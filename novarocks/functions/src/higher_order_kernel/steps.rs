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

//! A kernel step prepares owned data and returns before the body borrows its host.
//! Invocation state alone observes body completions and selects required errors.

// Design: ADR-0270 (docs/adr/ADR-0270-collection-lambdas-use-bound-body-requests.md)
use std::{cell::RefCell, sync::Arc};

use allocator_api2::vec::Vec as HostVec;
use arrow_array::{ArrayRef, new_empty_array};

use super::{
    HigherOrderEvaluationInstance, HigherOrderOutput, PreparedHigherOrderKernel,
    validate_prepared_metadata,
};
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::higher_order_call::validate_body_output;
use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::{
    AggregateStateAllocator, EvaluatedArgument, HigherOrderCallContract, KernelEvaluationControl,
    KernelFailure, LambdaBodyContract, LambdaElementRowMap, RequiredLambdaBodyError, RowDataError,
    SelectedValues, Selection,
};

/// The producer supplies each actual parameter's source array and one physical
/// index per requested element. Parent ordinals address the compact outer
/// selection. Captures are resolved by the evaluating host in that outer scope.
#[derive(Debug)]
pub struct LambdaBodyBatch {
    parents: HostVec<usize, HostAggregateAllocator>,
    parameters: HostVec<ArrayRef, HostAggregateAllocator>,
    positions: HostVec<usize, HostAggregateAllocator>,
}

impl LambdaBodyBatch {
    pub fn parent_ordinals(&self) -> &[usize] {
        &self.parents
    }

    pub fn parameter_sources(&self) -> &[ArrayRef] {
        &self.parameters
    }

    pub fn parameter_position(&self, parameter: usize, row: usize) -> Option<usize> {
        if parameter >= self.parameters.len() || row >= self.parents.len() {
            return None;
        }
        Some(self.positions[row * self.parameters.len() + parameter])
    }

    fn validate(
        &self,
        body: &LambdaBodyContract,
        outer: Selection<'_>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        if self.parents.is_empty() || self.parameters.len() != body.parameter_types().len() {
            return Err(invalid(
                "lambda batch does not have its required element domain",
            ));
        }
        LambdaElementRowMap::try_new(outer, &self.parents, control)?;
        let mut work = EvaluationCheckpoints::new(control);
        for (position, (source, ty)) in self
            .parameters
            .iter()
            .zip(body.parameter_types())
            .enumerate()
        {
            let rows = self
                .positions
                .chunks_exact(self.parameters.len())
                .map(|row| row[position]);
            let work = RefCell::new(&mut work);
            novarocks_value_arrays::hydrate::validate_required_rows_observed(
                source,
                ty.typed_binding(),
                novarocks_type_contract::CarrierNullabilityPolicy::Permissive,
                rows,
                &mut |units| {
                    for _ in 0..units {
                        work.borrow_mut().step()?;
                    }
                    Ok(())
                },
                &mut |_| work.borrow_mut().step(),
            )
            .map_err(|error| match error {
                novarocks_value_arrays::ValueFailure::Caller(error) => error,
                novarocks_value_arrays::ValueFailure::Value(error) => {
                    KernelFailure::from_domain(error)
                }
            })?;
        }
        work.finish()
    }
}

/// Batch index allocation goes directly to the original host. Its request
/// capacity records this batch's known geometry and is local to the builder.
pub struct LambdaBodyBatchBuilder {
    batch: LambdaBodyBatch,
    row_capacity: usize,
}

impl LambdaBodyBatchBuilder {
    pub fn try_new(
        allocator: &Arc<dyn AggregateStateAllocator>,
        parameters: &[ArrayRef],
        row_capacity: usize,
    ) -> Result<Self, KernelFailure> {
        let position_capacity = row_capacity
            .checked_mul(parameters.len())
            .ok_or(KernelFailure::ResourceExhausted)?;
        let allocator = HostAggregateAllocator::try_new(Arc::clone(allocator))?;
        let parents = reserve_batch_storage(row_capacity, &allocator)?;
        let positions = reserve_batch_storage(position_capacity, &allocator)?;
        let mut sources = reserve_batch_storage(parameters.len(), &allocator)?;
        sources.extend(parameters.iter().cloned());
        Ok(Self {
            batch: LambdaBodyBatch {
                parents,
                parameters: sources,
                positions,
            },
            row_capacity,
        })
    }

    pub fn push(&mut self, parent: usize, positions: &[usize]) -> Result<(), KernelFailure> {
        if self.batch.parents.len() >= self.row_capacity {
            return Err(internal("lambda batch exceeded its reserved row capacity"));
        }
        if positions.len() != self.batch.parameters.len()
            || positions
                .iter()
                .zip(&self.batch.parameters)
                .any(|(&position, array)| position >= array.len())
        {
            return Err(invalid(
                "lambda parameter position is outside its actual source",
            ));
        }
        self.batch.parents.push(parent);
        self.batch.positions.extend_from_slice(positions);
        Ok(())
    }

    pub fn finish(self) -> LambdaBodyBatch {
        self.batch
    }
}

fn reserve_batch_storage<T>(
    capacity: usize,
    allocator: &HostAggregateAllocator,
) -> Result<HostVec<T, HostAggregateAllocator>, KernelFailure> {
    let mut values = HostVec::new_in(allocator.clone());
    values.try_reserve_exact(capacity).map_err(|failure| {
        allocator.recorded_failure().unwrap_or_else(|| {
            KernelFailure::from_domain(crate::allocation_failure::AllocationRefusal::new(
                "lambda batch scratch allocation",
                failure,
            ))
        })
    })?;
    Ok(values)
}

/// Per-invocation kernel state. The producer uses its host allocation authority
/// for any state that stays live across a returned request. A body response is
/// moved into this method exactly once.
pub trait HigherOrderCall<'input> {
    fn advance(
        &mut self,
        completed: Option<LambdaBodyBatchOutput<'input>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<HigherOrderStep<'input>, KernelFailure>;
    /// Exact retained bytes of the underlying instance, excluding the call's
    /// independently funded invocation-local state and batch backing.
    fn instance_retained_bytes(&self) -> usize;
}

pub enum HigherOrderStep<'input> {
    Request(LambdaBodyBatch),
    Complete(HigherOrderOutput<'input>),
}

/// An authentic body completion, moved into the next kernel step. The
/// retained request is available to reduce original positions or select row
/// errors. Its allocation and the body's output live through this reduction.
#[derive(Debug)]
pub struct LambdaBodyBatchOutput<'input> {
    batch: LambdaBodyBatch,
    outer: Selection<'input>,
    body: Arc<LambdaBodyContract>,
    origin: HostAggregateAllocator,
    values: ArrayRef,
    errors: Box<[RowDataError]>,
}

impl<'input> LambdaBodyBatchOutput<'input> {
    pub fn batch(&self) -> &LambdaBodyBatch {
        &self.batch
    }

    pub fn values(&self) -> &ArrayRef {
        &self.values
    }

    pub fn errors(&self) -> &[RowDataError] {
        &self.errors
    }

    pub fn required_parent_error(
        &self,
        error_index: usize,
    ) -> Result<Option<RequiredLambdaBodyError<'input>>, KernelFailure> {
        let Some(error) = self.errors.get(error_index) else {
            return Ok(None);
        };
        let &parent = self
            .batch
            .parent_ordinals()
            .get(error.selected_ordinal())
            .ok_or_else(|| internal("lambda error lost its selected parent"))?;
        Ok(Some(RequiredLambdaBodyError::from_batch(
            Arc::clone(&self.body),
            self.outer,
            parent,
            error,
            self.origin.clone(),
        )))
    }
}

/// An exact body request visible while its obligation is being consumed.
pub struct LambdaBodyDemand<'request> {
    body: &'request Arc<LambdaBodyContract>,
    batch: &'request LambdaBodyBatch,
    row_map: LambdaElementRowMap<'request>,
}

impl<'request> LambdaBodyDemand<'request> {
    pub fn contract(&self) -> &'request Arc<LambdaBodyContract> {
        self.body
    }

    pub const fn row_map(&self) -> LambdaElementRowMap<'request> {
        self.row_map
    }

    pub fn batch(&self) -> &'request LambdaBodyBatch {
        self.batch
    }

    /// Complete with values from the actual body and this exact demand.
    /// The output is opaque until the issuing obligation authenticates it.
    pub fn complete(
        self,
        body: &Arc<LambdaBodyContract>,
        output: SelectedValues<'request>,
    ) -> Result<LambdaBodyResult<'request>, KernelFailure> {
        if !Arc::ptr_eq(body, self.body) {
            return Err(invalid(
                "lambda evaluator belongs to a different compiled body",
            ));
        }
        Ok(LambdaBodyResult {
            batch: self.batch,
            output,
        })
    }
}

pub struct LambdaBodyResult<'request> {
    batch: &'request LambdaBodyBatch,
    output: SelectedValues<'request>,
}

/// A checked obligation. Dropping it leaves the invocation closed. Consuming
/// it runs the original body callback once and publishes one checked response.
/// An error from the body keeps its original type and leaves no continuation.
#[must_use = "the lambda body request must be consumed to continue the invocation"]
pub struct LambdaBodyRequestObligation<'step, 'input> {
    continuation: &'step mut Continuation<'input>,
    batch: LambdaBodyBatch,
    outer: Selection<'input>,
    body: &'step Arc<LambdaBodyContract>,
    origin: &'step HostAggregateAllocator,
}

impl LambdaBodyRequestObligation<'_, '_> {
    pub fn run_with<E: From<KernelFailure>>(
        self,
        control: &dyn KernelEvaluationControl,
        run_body: impl for<'request> FnOnce(
            LambdaBodyDemand<'request>,
        ) -> Result<LambdaBodyResult<'request>, E>,
    ) -> Result<(), E> {
        control.checkpoint(0)?;
        let row_map = LambdaElementRowMap::from_validated(self.outer, &self.batch.parents);
        let result = run_body(LambdaBodyDemand {
            body: self.body,
            batch: &self.batch,
            row_map,
        })?;
        validate_completion_batch(&self.batch, &result)?;
        validate_body_output(self.body, row_map.all_elements(), &result.output, control)?;
        control.checkpoint(0)?;
        let (_, values, errors) = result.output.into_parts();
        *self.continuation = Continuation::Body(LambdaBodyBatchOutput {
            batch: self.batch,
            outer: self.outer,
            body: Arc::clone(self.body),
            origin: self.origin.clone(),
            values,
            errors,
        });
        Ok(())
    }
}

fn validate_completion_batch(
    expected: &LambdaBodyBatch,
    completed: &LambdaBodyResult<'_>,
) -> Result<(), KernelFailure> {
    if !std::ptr::eq(completed.batch, expected) {
        return Err(invalid(
            "lambda completion belongs to a different body batch",
        ));
    }
    Ok(())
}

enum Continuation<'input> {
    Start,
    Body(LambdaBodyBatchOutput<'input>),
    Closed,
}

pub enum HigherOrderEvaluationStep<'step, 'input> {
    Request(LambdaBodyRequestObligation<'step, 'input>),
    Complete(SelectedValues<'input>),
}

/// One selected invocation. A request exclusively borrows its continuation;
/// the producer cannot advance another step until that request is consumed.
pub struct HigherOrderInvocation<'call, 'input> {
    call: Option<Box<dyn HigherOrderCall<'input> + 'call>>,
    continuation: Continuation<'input>,
    selection: Selection<'input>,
    contract: &'call Arc<HigherOrderCallContract>,
    body: &'call Arc<LambdaBodyContract>,
    prepared: &'call Arc<dyn PreparedHigherOrderKernel>,
    retained_bound: usize,
    origin: Option<HostAggregateAllocator>,
    failed: &'call mut bool,
}

impl<'input> HigherOrderInvocation<'_, 'input> {
    pub fn advance(
        &mut self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<HigherOrderEvaluationStep<'_, 'input>, KernelFailure> {
        let completed = match std::mem::replace(&mut self.continuation, Continuation::Closed) {
            Continuation::Start => None,
            Continuation::Body(completed) => Some(completed),
            Continuation::Closed => return Err(KernelFailure::InstanceFailed),
        };
        control.checkpoint(0)?;
        let Some(call) = &mut self.call else {
            let result_type = self.contract.result_type().data_type();
            let output = SelectedValues::try_new(
                self.selection,
                result_type,
                new_empty_array(result_type),
                Box::default(),
            )
            .map_err(|_| internal("empty higher-order result violates its exact type"))?;
            *self.failed = false;
            return Ok(HigherOrderEvaluationStep::Complete(output));
        };
        let observed = crate::KernelControlObservation::new(control);
        let step = observed.finish(call.advance(completed, &observed))?;
        validate_prepared_metadata(self.prepared, self.contract, self.body, self.retained_bound)?;
        if call.instance_retained_bytes() > self.retained_bound {
            return Err(internal(
                "higher-order instance exceeded its lifetime retained bound",
            ));
        }
        control.checkpoint(0)?;
        match step {
            HigherOrderStep::Request(batch) => {
                batch.validate(self.body, self.selection, control)?;
                Ok(HigherOrderEvaluationStep::Request(
                    LambdaBodyRequestObligation {
                        continuation: &mut self.continuation,
                        batch,
                        outer: self.selection,
                        body: self.body,
                        origin: self
                            .origin
                            .as_ref()
                            .expect("a live invocation has an allocation identity"),
                    },
                ))
            }
            HigherOrderStep::Complete(output) => {
                super::validate_output(
                    self.contract,
                    self.body,
                    self.selection,
                    &output,
                    self.origin.as_ref(),
                    control,
                )?;
                self.call = None;
                *self.failed = false;
                Ok(HigherOrderEvaluationStep::Complete(output.values))
            }
        }
    }
}

impl HigherOrderEvaluationInstance {
    pub fn begin<'call, 'input: 'call>(
        &'call mut self,
        selection: Selection<'input>,
        arguments: &'input [EvaluatedArgument<'input>],
        allocator: &Arc<dyn AggregateStateAllocator>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<HigherOrderInvocation<'call, 'input>, KernelFailure> {
        if self.failed {
            return Err(KernelFailure::InstanceFailed);
        }
        // Only a validated final step makes this instance reusable.
        self.failed = true;
        control.checkpoint(0)?;
        self.validate_metadata()?;
        self.retained_bytes()?;
        super::validate_ordinary_inputs(&self.contract, selection, arguments, control)?;
        let (call, origin) = if selection.is_empty() {
            (None, None)
        } else {
            let origin = HostAggregateAllocator::try_new(Arc::clone(allocator))?;
            let input = super::HigherOrderCallInput {
                contract: &self.contract,
                selection,
                arguments,
                #[cfg(test)]
                captures: &[],
            };
            let observed = crate::KernelControlObservation::new(control);
            let call = observed.finish(self.instance.begin(input, allocator, &observed))?;
            validate_prepared_metadata(
                &self.prepared,
                &self.contract,
                &self.body,
                self.retained_bound,
            )?;
            if call.instance_retained_bytes() > self.retained_bound {
                return Err(internal(
                    "higher-order instance exceeded its lifetime retained bound",
                ));
            }
            control.checkpoint(0)?;
            (Some(call), Some(origin))
        };
        Ok(HigherOrderInvocation {
            call,
            continuation: Continuation::Start,
            selection,
            contract: &self.contract,
            body: &self.body,
            prepared: &self.prepared,
            retained_bound: self.retained_bound,
            origin,
            failed: &mut self.failed,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::DataType;
    use std::{
        alloc::Layout,
        ptr::NonNull,
        sync::atomic::{AtomicUsize, Ordering},
    };

    #[derive(Default)]
    struct BatchHost {
        live: AtomicUsize,
        attempts: AtomicUsize,
        refuse: Option<usize>,
    }

    fn source_failure() -> KernelFailure {
        KernelFailure::Operational(crate::KernelDiagnostic::new(
            "original batch backing refusal",
        ))
    }

    impl AggregateStateAllocator for BatchHost {
        fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
            let attempt = self.attempts.fetch_add(1, Ordering::Relaxed);
            if self.refuse == Some(attempt) {
                return Err(source_failure());
            }
            let block = crate::UnaccountedAggregateStateAllocator.allocate(layout)?;
            self.live.fetch_add(layout.size(), Ordering::Relaxed);
            Ok(block)
        }

        unsafe fn release(&self, block: NonNull<u8>, layout: Layout) {
            self.live.fetch_sub(layout.size(), Ordering::Relaxed);
            // The test only releases exact blocks accepted by the heap host.
            unsafe { crate::UnaccountedAggregateStateAllocator.release(block, layout) };
        }
    }

    #[test]
    fn higher_order_batch_owns_actual_storage_and_preserves_mid_construction_refusal() {
        let source: ArrayRef = Arc::new(Int64Array::from(vec![42]));
        for refuse in [None, Some(2)] {
            let host = Arc::new(BatchHost {
                refuse,
                ..BatchHost::default()
            });
            let allocator: Arc<dyn AggregateStateAllocator> = host.clone();
            let builder =
                LambdaBodyBatchBuilder::try_new(&allocator, std::slice::from_ref(&source), 1);
            if refuse.is_some() {
                assert!(matches!(builder, Err(ref failure) if *failure == source_failure()));
                assert_eq!(host.attempts.load(Ordering::Relaxed), 3);
            } else {
                let mut builder = builder.unwrap();
                builder.push(0, &[0]).unwrap();
                let batch = builder.finish();
                assert!(host.live.load(Ordering::Relaxed) > 0);
                assert_eq!(Arc::strong_count(&source), 2);
                drop(batch);
            }
            assert_eq!(host.live.load(Ordering::Relaxed), 0);
            assert_eq!(Arc::strong_count(&source), 1);
        }
    }

    #[test]
    fn higher_order_completion_rejects_equal_geometry_from_another_frame() {
        let host: Arc<dyn AggregateStateAllocator> =
            Arc::new(crate::UnaccountedAggregateStateAllocator);
        let source: ArrayRef = Arc::new(Int64Array::from(vec![42]));
        let request = || {
            let mut builder =
                LambdaBodyBatchBuilder::try_new(&host, &[Arc::clone(&source)], 1).unwrap();
            builder.push(0, &[0]).unwrap();
            builder.finish()
        };
        let first = request();
        let other = request();
        assert_eq!(first.parent_ordinals(), other.parent_ordinals());
        assert_eq!(
            first.parameter_position(0, 0),
            other.parameter_position(0, 0)
        );
        assert!(Arc::ptr_eq(
            &first.parameter_sources()[0],
            &other.parameter_sources()[0]
        ));
        let completed = LambdaBodyResult {
            batch: &first,
            output: SelectedValues::try_new(
                Selection::all(1),
                &DataType::Int64,
                source,
                Box::new([]),
            )
            .unwrap(),
        };
        assert!(matches!(
            validate_completion_batch(&other, &completed),
            Err(KernelFailure::InvalidProgram(_)),
        ));
        validate_completion_batch(&first, &completed).unwrap();
    }
}
