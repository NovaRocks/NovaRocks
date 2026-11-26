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

use crate::opaque_memory::OpaqueAllocationHost;
use crate::{AggregateStateAllocator, KernelDiagnostic, KernelEvaluationControl, KernelFailure};
use std::{
    alloc::Layout,
    ptr::NonNull,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Default)]
pub(super) struct Ledger {
    attempts: usize,
    blocks: Vec<(usize, Layout)>,
    opaque: usize,
    opaque_attempts: usize,
}
pub(super) struct Host {
    ledger: Mutex<Ledger>,
    actual_limit: usize,
    pub(super) refusal: Mutex<Option<(usize, KernelFailure)>>,
    pub(super) opaque_refusal: Mutex<Option<(usize, KernelFailure)>>,
}
impl Host {
    pub(super) fn with_limit(actual_limit: usize) -> Arc<Self> {
        Arc::new(Self {
            ledger: Mutex::new(Ledger::default()),
            actual_limit,
            refusal: Mutex::new(None),
            opaque_refusal: Mutex::new(None),
        })
    }
    pub(super) fn attempts(&self) -> usize {
        self.ledger.lock().unwrap().attempts
    }
    pub(super) fn released(&self) {
        let l = self.ledger.lock().unwrap();
        assert!(l.blocks.is_empty());
        assert_eq!(l.opaque, 0);
    }
}
impl AggregateStateAllocator for Host {
    fn opaque_allocation_host(&self) -> Option<&dyn OpaqueAllocationHost> {
        Some(self)
    }
    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
        assert_ne!(layout.size(), 0);
        let mut l = self.ledger.lock().unwrap();
        let at = l.attempts;
        l.attempts += 1;
        if let Some((stop, cause)) = &*self.refusal.lock().unwrap() {
            assert!(at <= *stop, "host request after first refusal");
            if at == *stop {
                return Err(cause.clone());
            }
        }
        if l.opaque + l.blocks.iter().map(|(_, v)| v.size()).sum::<usize>() + layout.size()
            > self.actual_limit
        {
            return Err(KernelFailure::ResourceExhausted);
        }
        let ptr = NonNull::new(unsafe { std::alloc::alloc(layout) })
            .ok_or(KernelFailure::ResourceExhausted)?;
        l.blocks.push((ptr.as_ptr().addr(), layout));
        Ok(ptr)
    }
    unsafe fn release(&self, pointer: NonNull<u8>, layout: Layout) {
        let mut l = self.ledger.lock().unwrap();
        let at = l
            .blocks
            .iter()
            .position(|(p, t)| *p == pointer.as_ptr().addr() && *t == layout)
            .expect("exact live host block");
        l.blocks.swap_remove(at);
        unsafe { std::alloc::dealloc(pointer.as_ptr(), layout) };
    }
}
impl OpaqueAllocationHost for Host {
    fn reserve_opaque(&self, bytes: usize) -> Result<(), KernelFailure> {
        let mut l = self.ledger.lock().unwrap();
        let at = l.opaque_attempts;
        l.opaque_attempts += 1;
        if let Some((stop, cause)) = &*self.opaque_refusal.lock().unwrap() {
            assert!(at <= *stop, "host request after first refusal");
            if at == *stop {
                return Err(cause.clone());
            }
        }
        if l.opaque + l.blocks.iter().map(|(_, v)| v.size()).sum::<usize>() + bytes
            > self.actual_limit
        {
            return Err(KernelFailure::ResourceExhausted);
        }
        l.opaque += bytes;
        Ok(())
    }
    fn release_opaque(&self, bytes: usize) {
        let mut l = self.ledger.lock().unwrap();
        assert!(bytes <= l.opaque);
        l.opaque -= bytes;
    }
}
#[derive(Default)]
pub(super) struct Control {
    pub(super) trace: Mutex<Vec<u32>>,
    pub(super) refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut t = self.trace.lock().unwrap();
        let at = t.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "callback after first cause");
        }
        t.push(units);
        match &self.refusal {
            Some((stop, cause)) if at == *stop => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("array invocation never waits")
    }
}
pub(super) fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("original host cause")),
        KernelFailure::Internal(KernelDiagnostic::new("original host cause")),
        KernelFailure::Operational(KernelDiagnostic::new("original host cause")),
        KernelFailure::InstanceFailed,
    ]
}

impl Host {
    pub(super) fn arm_allocation(&self, offset: usize, cause: KernelFailure) {
        let at = self.ledger.lock().unwrap().attempts;
        *self.refusal.lock().unwrap() = Some((at + offset, cause));
    }
    pub(super) fn arm_opaque(&self, offset: usize, cause: KernelFailure) {
        let at = self.ledger.lock().unwrap().opaque_attempts;
        *self.opaque_refusal.lock().unwrap() = Some((at + offset, cause));
    }
    pub(super) fn opaque_attempts(&self) -> usize {
        self.ledger.lock().unwrap().opaque_attempts
    }
    fn opaque_bytes(&self) -> usize {
        self.ledger.lock().unwrap().opaque
    }
}

impl Host {
    pub(super) fn live_blocks(&self) -> usize {
        self.ledger.lock().unwrap().blocks.len()
    }
}

/// Exercise the actual catalogue-owned preparation and original scalar wrapper
/// at the same sparse selection. Depend on the row's value, not on how its
/// source backing was repeated.
pub(super) fn assert_scalar_broadcast_equivalent(
    name: &str,
    inputs: &[(&arrow_array::ArrayRef, usize)],
    expect_row_error: bool,
) {
    use crate::selected_copy::{CopyIndices, CopyOperationError, take_copy_in};
    use crate::{
        CallArgumentUses, CallEffectInput, EvaluatedArgument, FunctionArgument,
        FunctionBindingRequest, FunctionKind, FunctionValueType, PreparedPureKernel,
        PureCallPreparation, ScalarEvaluationInstance, ScopedExpressionEffects, Selection,
    };
    use arrow_array::UInt64Array;
    use novarocks_type_contract::{
        CallProofScope, DecimalOverflowPolicy, EvaluationDemand, EvaluationRegionId,
        ExpressionEffectContext, ExpressionUseId, SemanticParameters,
    };
    use std::error::Error as _;

    let catalog = super::catalogue::builtin_engine_function_catalog();
    let arguments = inputs
        .iter()
        .map(|(array, _)| FunctionArgument::Value {
            value_type: FunctionValueType::from_legacy_declared_projection(
                array.data_type().clone(),
                true,
            ),
            constant: None,
        })
        .collect::<Vec<_>>();
    let request = FunctionBindingRequest {
        arguments: &arguments,
        logical_argument_count: arguments.len(),
        expected_result_type: None,
    };
    let compile = crate::binding_test_control();
    let bound = catalog
        .resolve_bound_user(name, FunctionKind::Scalar, request, compile)
        .unwrap();
    let selected = Arc::new(bound.selected);
    let cx = ExpressionEffectContext {
        use_id: ExpressionUseId::new(1),
        region: EvaluationRegionId::new(1),
        demand: EvaluationDemand::Value,
    };
    let uses = (2..2 + arguments.len() as u32)
        .map(|id| Some(ExpressionUseId::new(id)))
        .collect::<Vec<_>>();
    let prepared = catalog
        .prepare_fresh_selected(
            CallEffectInput {
                function_id: &bound.function_id,
                kind: FunctionKind::Scalar,
                selected: &selected,
                context: cx.into(),
                argument_uses: CallArgumentUses::SelectedChannels(&uses),
                request,
                environment: &[],
                parameters: &SemanticParameters::default(),
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                proof_scope: CallProofScope::Unconditional,
            },
            selected.clone(),
            PureCallPreparation::Scalar {
                arguments: ScopedExpressionEffects::pure_value(cx),
            },
            compile,
        )
        .unwrap();
    let PreparedPureKernel::Scalar(kernel) = prepared.prepared() else {
        panic!("expected the original scalar lifecycle");
    };
    for ordinal in 0..arguments.len() {
        assert!(
            kernel.supports_scalar_broadcast(ordinal),
            "{name}, ordinal {ordinal}"
        );
    }
    assert!(!kernel.supports_scalar_broadcast(arguments.len()));

    let selection = Selection::try_sparse(7, &[1, 4, 6]).unwrap();
    let scalars = inputs
        .iter()
        .map(|(source, row)| source.slice(*row, 1))
        .collect::<Vec<_>>();
    let repeated =
        CopyIndices::UInt64(Arc::new(UInt64Array::from(vec![0; selection.batch_rows()])));
    let refused = Host::with_limit(usize::MAX);
    refused.arm_opaque(0, KernelFailure::ResourceExhausted);
    assert!(matches!(
        take_copy_in(
            scalars[0].clone(),
            repeated.clone(),
            (),
            refused.clone(),
            &Control::default()
        ),
        Err(CopyOperationError::Control(
            KernelFailure::ResourceExhausted
        ))
    ));
    refused.released();

    let invoke = |mask| {
        let is_scalar = |ordinal| (1usize << ordinal) & mask != 0;
        let copy_host = Host::with_limit(usize::MAX);
        let columns = scalars
            .iter()
            .enumerate()
            .map(|(ordinal, scalar)| {
                (!is_scalar(ordinal)).then(|| {
                    take_copy_in(
                        scalar.clone(),
                        repeated.clone(),
                        (),
                        copy_host.clone(),
                        &Control::default(),
                    )
                    .unwrap()
                })
            })
            .collect::<Vec<_>>();
        let copied_bytes: usize = columns
            .iter()
            .flatten()
            .map(|column| column.new_buffer_stock())
            .sum();
        let args = scalars
            .iter()
            .zip(&columns)
            .map(|(scalar, column)| match column {
                Some(column) => EvaluatedArgument::Column(column.values()),
                None => EvaluatedArgument::Scalar(scalar),
            })
            .collect::<Vec<_>>();
        let host = Host::with_limit(usize::MAX);
        let mut instance = ScalarEvaluationInstance::instantiate_with_allocator(
            Arc::clone(kernel),
            Some(host.clone()),
        )
        .unwrap();
        let output = instance
            .evaluate(selection, &args, &Control::default())
            .unwrap();
        let rejected = Control {
            refusal: Some((0, KernelFailure::DeadlineExceeded)),
            ..Control::default()
        };
        assert!(matches!(
            instance.evaluate(selection, &args, &rejected),
            Err(KernelFailure::DeadlineExceeded)
        ));
        assert!(matches!(
            instance.evaluate(selection, &args, &Control::default()),
            Err(KernelFailure::InstanceFailed)
        ));
        assert_eq!(output.selection(), selection);
        let alias = output.values().clone();
        let errors = output.errors().to_vec();
        drop(output);
        drop(instance);
        (host, alias, errors, copy_host, copied_bytes)
    };
    let (baseline_host, baseline, baseline_errors, baseline_copy_host, baseline_copy_bytes) =
        invoke(0);
    assert!(baseline_copy_bytes > 0);
    assert_eq!(
        baseline_errors.len(),
        if expect_row_error { selection.len() } else { 0 }
    );
    for (ordinal, error) in baseline_errors.iter().enumerate() {
        assert_eq!(error.selected_ordinal(), ordinal);
        assert!(baseline.is_null(ordinal));
    }
    let retained = baseline_host.opaque_bytes();
    for mask in 1..(1 << arguments.len()) {
        let (host, output, errors, copy_host, copied_bytes) = invoke(mask);
        assert_eq!(
            output.data_type(),
            kernel.contract().result_type().data_type()
        );
        assert_eq!(output.to_data(), baseline.to_data());
        assert_eq!(errors, baseline_errors);
        for (expected, actual) in baseline_errors.iter().zip(&errors) {
            assert_eq!(
                expected.source().map(ToString::to_string),
                actual.source().map(ToString::to_string),
            );
        }
        assert_eq!(host.opaque_bytes(), retained);
        assert!(copied_bytes <= baseline_copy_bytes);
        if mask == (1 << arguments.len()) - 1 {
            assert_eq!(copied_bytes, 0);
            assert_eq!(copy_host.opaque_attempts(), 0);
        }
        drop(output);
        drop(errors);
        host.released();
        copy_host.released();
    }
    drop(baseline);
    drop(baseline_errors);
    baseline_host.released();
    baseline_copy_host.released();
}
