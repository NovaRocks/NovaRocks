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

use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use novarocks_functions::AggregateInputBatch;

use crate::exec::expr::agg::{
    AggKernelEntry, AggStatePtr, AggregateAllocator, AggregateVec, RetainedMemoryPolicy,
};
use crate::runtime::mem_tracker::{MemTracker, process_mem_tracker};

const RETAINED_MEMORY_CHECK_ROWS: usize = 64;

pub(super) struct AggregateRetainedMemory {
    tracker: Option<Arc<MemTracker>>,
    bytes: usize,
    memory_limit_exceeded: bool,
}

impl AggregateRetainedMemory {
    pub(super) const fn new() -> Self {
        Self {
            tracker: None,
            bytes: 0,
            memory_limit_exceeded: false,
        }
    }

    pub(super) fn set_tracker(&mut self, tracker: Arc<MemTracker>) -> Result<(), String> {
        if let Some(current) = self.tracker.as_ref() {
            if Arc::ptr_eq(current, &tracker) {
                return Ok(());
            }
            return Err("aggregate retained-memory tracker cannot be rebound".to_string());
        }
        if self.bytes != 0 {
            return Err(
                "aggregate retained-memory tracker must be bound before allocation".to_string(),
            );
        }
        self.tracker = Some(tracker);
        Ok(())
    }

    pub(super) fn initialize_state(
        &mut self,
        kernel: &AggKernelEntry,
        state: AggStatePtr,
    ) -> Result<(), String> {
        self.ensure_available()?;
        self.ensure_tracker_bound();
        match kernel.retained_memory_policy() {
            RetainedMemoryPolicy::FixedZero => {
                kernel.init_state(state)?;
                let retained = kernel.retained_bytes(state);
                if retained == 0 {
                    return Ok(());
                }
                let accounting = self.apply_live_growth(retained);
                kernel.drop_state(state);
                self.release_accounted(retained);
                self.memory_limit_exceeded = true;
                let violation = format!(
                    "fixed-zero aggregate retained {retained} bytes after state initialization"
                );
                match accounting {
                    Ok(()) => Err(violation),
                    Err(error) => Err(format!("{violation}; accounting also failed: {error}")),
                }
            }
            RetainedMemoryPolicy::AllocationTracked => {
                let Some(tracker) = self.tracker.as_ref().map(Arc::clone) else {
                    return Err(
                        "allocation-tracked aggregate requires a bound memory tracker".to_string(),
                    );
                };
                kernel.init_state_with_tracker(state, tracker)
            }
            RetainedMemoryPolicy::BoundedRetained {
                max_retained_bytes_per_state,
            } => {
                self.reserve_headroom(max_retained_bytes_per_state)?;
                if let Err(error) = kernel.init_state(state) {
                    self.release_accounted(max_retained_bytes_per_state);
                    return Err(error);
                }
                let retained = kernel.retained_bytes(state);
                if retained <= max_retained_bytes_per_state {
                    self.release_accounted(max_retained_bytes_per_state - retained);
                    return Ok(());
                }

                let excess = retained - max_retained_bytes_per_state;
                let accounting = self.apply_live_growth(excess);
                kernel.drop_state(state);
                self.release_accounted(retained);
                self.memory_limit_exceeded = true;
                let violation = format!(
                    "bounded aggregate retained {retained} bytes after state initialization, exceeding declared maximum {max_retained_bytes_per_state}"
                );
                match accounting {
                    Ok(()) => Err(violation),
                    Err(error) => Err(format!("{violation}; accounting also failed: {error}")),
                }
            }
        }
    }

    pub(super) fn around<T>(
        &mut self,
        kernel: &AggKernelEntry,
        states: &[AggStatePtr],
        operation: impl FnOnce() -> Result<T, String>,
    ) -> Result<T, String> {
        self.ensure_available()?;
        match kernel.retained_memory_policy() {
            RetainedMemoryPolicy::AllocationTracked => operation(),
            RetainedMemoryPolicy::FixedZero => self.around_fixed_zero(kernel, states, operation),
            RetainedMemoryPolicy::BoundedRetained {
                max_retained_bytes_per_state,
            } => {
                // Output materialization receives shared state references in the
                // typed contract and therefore cannot require mutation
                // headroom. Still verify the declared retained-memory contract
                // around legacy erased implementations.
                self.around_read_only_bounded(
                    kernel,
                    states,
                    max_retained_bytes_per_state,
                    operation,
                )
            }
        }
    }

    pub(super) fn run_bounded_batch(
        &mut self,
        touched: &mut TouchedAggregateStates,
        kernel: &AggKernelEntry,
        state_ptrs: &[AggStatePtr],
        batch: AggregateInputBatch<'_>,
        merge: bool,
    ) -> Result<(), String> {
        if state_ptrs.len() != batch.row_count() {
            return Err(format!(
                "aggregate bounded batch state count mismatch: states={}, rows={}",
                state_ptrs.len(),
                batch.row_count()
            ));
        }
        match kernel.retained_memory_policy() {
            RetainedMemoryPolicy::AllocationTracked => {
                self.ensure_available()?;
                return if merge {
                    kernel.merge_batch(state_ptrs, batch)
                } else {
                    kernel.update_batch(state_ptrs, batch)
                };
            }
            RetainedMemoryPolicy::FixedZero => {
                return self.around_mutation(kernel, state_ptrs, || {
                    if merge {
                        kernel.merge_batch(state_ptrs, batch)
                    } else {
                        kernel.update_batch(state_ptrs, batch)
                    }
                });
            }
            RetainedMemoryPolicy::BoundedRetained { .. } => {}
        }
        for start in (0..batch.row_count()).step_by(RETAINED_MEMORY_CHECK_ROWS) {
            let len = RETAINED_MEMORY_CHECK_ROWS.min(batch.row_count() - start);
            let values = batch.array_ref().map(|values| values.slice(start, len));
            let slice = AggregateInputBatch::try_new(values.as_ref(), len)
                .map_err(|error| error.to_string())?;
            let state_slice = &state_ptrs[start..start + len];
            let touched = touched.pointers(state_slice)?;
            self.around_mutation(kernel, touched, || {
                if merge {
                    kernel.merge_batch(state_slice, slice)
                } else {
                    kernel.update_batch(state_slice, slice)
                }
            })?;
        }
        Ok(())
    }

    pub(super) fn drop_initialized_state(&mut self, kernel: &AggKernelEntry, state: AggStatePtr) {
        match kernel.retained_memory_policy() {
            RetainedMemoryPolicy::AllocationTracked => kernel.drop_state(state),
            RetainedMemoryPolicy::FixedZero => {
                let retained = kernel.retained_bytes(state);
                kernel.drop_state(state);
                if retained != 0 {
                    self.release_accounted(retained);
                }
            }
            RetainedMemoryPolicy::BoundedRetained { .. } => {
                let retained = kernel.retained_bytes(state);
                kernel.drop_state(state);
                self.release_accounted(retained);
            }
        }
    }

    pub(super) fn release_all(&mut self) {
        if let Some(tracker) = self.tracker.as_ref() {
            tracker.release(bytes_to_i64(self.bytes));
        }
        self.bytes = 0;
    }

    fn around_mutation<T>(
        &mut self,
        kernel: &AggKernelEntry,
        states: &[AggStatePtr],
        operation: impl FnOnce() -> Result<T, String>,
    ) -> Result<T, String> {
        match kernel.retained_memory_policy() {
            RetainedMemoryPolicy::AllocationTracked => operation(),
            RetainedMemoryPolicy::FixedZero => self.around_fixed_zero(kernel, states, operation),
            RetainedMemoryPolicy::BoundedRetained {
                max_retained_bytes_per_state,
            } => self.around_bounded_mutation(
                kernel,
                states,
                max_retained_bytes_per_state,
                operation,
            ),
        }
    }

    fn around_fixed_zero<T>(
        &mut self,
        kernel: &AggKernelEntry,
        states: &[AggStatePtr],
        operation: impl FnOnce() -> Result<T, String>,
    ) -> Result<T, String> {
        let before = retained_sum(kernel, states)?;
        if before != 0 {
            self.memory_limit_exceeded = true;
            return Err(format!(
                "fixed-zero aggregate retained {before} bytes before operation"
            ));
        }
        let operation_result = operation();
        let after = retained_sum(kernel, states)?;
        if after == 0 {
            return operation_result;
        }
        let accounting_result = self.apply_live_growth(after);
        self.memory_limit_exceeded = true;
        let violation = format!("fixed-zero aggregate retained {after} bytes after operation");
        let contract_result = match accounting_result {
            Ok(()) => Err(violation),
            Err(error) => Err(format!("{violation}; accounting also failed: {error}")),
        };
        combine_operation_and_accounting(operation_result, contract_result)
    }

    fn around_read_only_bounded<T>(
        &mut self,
        kernel: &AggKernelEntry,
        states: &[AggStatePtr],
        max_retained_bytes_per_state: usize,
        operation: impl FnOnce() -> Result<T, String>,
    ) -> Result<T, String> {
        let before = retained_sum(kernel, states)?;
        let maximum = max_retained_bytes_per_state
            .checked_mul(states.len())
            .ok_or_else(|| "aggregate retained-memory bound overflowed usize".to_string())?;
        if before > maximum {
            self.memory_limit_exceeded = true;
            return Err(format!(
                "bounded aggregate retained {before} bytes before read-only operation, exceeding declared maximum {maximum}"
            ));
        }
        let operation_result = operation();
        let after = retained_sum(kernel, states)?;
        if before == after && after <= maximum {
            return operation_result;
        }
        let accounting_result = self.reconcile_live(before, after);
        self.memory_limit_exceeded = true;
        let violation = if after > maximum {
            Err(format!(
                "read-only bounded aggregate retained {after} bytes, exceeding declared maximum {maximum}"
            ))
        } else {
            Err(format!(
                "read-only aggregate operation changed retained memory from {before} to {after} bytes"
            ))
        };
        combine_operation_and_accounting(
            combine_operation_and_accounting(operation_result, accounting_result),
            violation,
        )
    }

    fn around_bounded_mutation<T>(
        &mut self,
        kernel: &AggKernelEntry,
        states: &[AggStatePtr],
        max_retained_bytes_per_state: usize,
        operation: impl FnOnce() -> Result<T, String>,
    ) -> Result<T, String> {
        let before = retained_sum(kernel, states)?;
        let maximum = max_retained_bytes_per_state
            .checked_mul(states.len())
            .ok_or_else(|| "aggregate retained-memory reservation overflowed usize".to_string())?;
        if before > maximum {
            self.memory_limit_exceeded = true;
            return Err(format!(
                "bounded aggregate retained {before} bytes before mutation, exceeding declared maximum {maximum}"
            ));
        }
        let headroom = maximum - before;
        self.reserve_headroom(headroom)?;
        let operation_result = operation();
        let after = retained_sum(kernel, states)?;
        let accounting_result = if after <= maximum {
            self.release_accounted(maximum - after);
            Ok(())
        } else {
            let accounting = self.apply_live_growth(after - maximum);
            self.memory_limit_exceeded = true;
            let violation = format!(
                "bounded aggregate retained {after} bytes after mutation, exceeding declared maximum {maximum}"
            );
            match accounting {
                Ok(()) => Err(violation),
                Err(error) => Err(format!("{violation}; accounting also failed: {error}")),
            }
        };
        combine_operation_and_accounting(operation_result, accounting_result)
    }

    fn reconcile_live(&mut self, before: usize, after: usize) -> Result<(), String> {
        if after >= before {
            self.apply_live_growth(after - before)
        } else {
            let released = before - after;
            self.release_accounted(released);
            Ok(())
        }
    }

    fn reserve_headroom(&mut self, bytes: usize) -> Result<(), String> {
        if bytes == 0 {
            return Ok(());
        }
        let next = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| "aggregate retained-memory reservation overflowed usize".to_string())?;
        if let Some(tracker) = self.tracker.as_ref()
            && let Err(error) = tracker.consume_and_check_limit(bytes_to_i64(bytes))
        {
            tracker.release(bytes_to_i64(bytes));
            self.memory_limit_exceeded = true;
            return Err(error);
        }
        self.bytes = next;
        Ok(())
    }

    fn apply_live_growth(&mut self, growth: usize) -> Result<(), String> {
        if growth == 0 {
            return Ok(());
        }
        self.bytes = self
            .bytes
            .checked_add(growth)
            .ok_or_else(|| "aggregate retained-memory accounting overflowed usize".to_string())?;
        if let Some(tracker) = self.tracker.as_ref() {
            // A limit error does not roll accounting back because the state
            // already owns the memory. Operator failure will drop the state
            // and release the complete retained total.
            if let Err(error) = tracker.consume_and_check_limit(bytes_to_i64(growth)) {
                self.memory_limit_exceeded = true;
                return Err(error);
            }
        }
        Ok(())
    }

    fn release_accounted(&mut self, released: usize) {
        if released == 0 {
            return;
        }
        self.bytes = self.bytes.checked_sub(released).unwrap_or_else(|| {
            panic!(
                "aggregate retained-memory accounting underflow: tracked={}, release={released}",
                self.bytes
            )
        });
        if let Some(tracker) = self.tracker.as_ref() {
            tracker.release(bytes_to_i64(released));
        }
    }

    pub(super) fn ensure_available(&self) -> Result<(), String> {
        if self.memory_limit_exceeded {
            Err("aggregate retained-memory limit was previously exceeded".to_string())
        } else {
            Ok(())
        }
    }

    fn ensure_tracker_bound(&mut self) {
        if self.tracker.is_none() {
            self.tracker = Some(MemTracker::new_child(
                "AggregateRetainedHeapUnbounded",
                &process_mem_tracker(),
            ));
        }
    }
}

impl Drop for AggregateRetainedMemory {
    fn drop(&mut self) {
        self.release_all();
    }
}

pub(super) struct TouchedAggregateStates {
    states: Option<AggregateVec<AggStatePtr>>,
    tracker: Option<Arc<MemTracker>>,
    memory_limit_exceeded: bool,
}

impl TouchedAggregateStates {
    pub(super) const fn new() -> Self {
        Self {
            states: None,
            tracker: None,
            memory_limit_exceeded: false,
        }
    }

    pub(super) fn set_tracker(&mut self, tracker: Arc<MemTracker>) -> Result<(), String> {
        if let Some(current) = self.tracker.as_ref() {
            if Arc::ptr_eq(current, &tracker) {
                return Ok(());
            }
            return Err("aggregate touched-state tracker cannot be rebound".to_string());
        }
        if self.states.is_some() {
            return Err("aggregate touched-state tracker cannot be rebound".to_string());
        }
        let allocator = AggregateAllocator::new(Arc::clone(&tracker));
        self.states = Some(AggregateVec::new_in(allocator));
        self.tracker = Some(tracker);
        Ok(())
    }

    pub(super) fn pointers(
        &mut self,
        state_ptrs: &[AggStatePtr],
    ) -> Result<&[AggStatePtr], String> {
        self.ensure_available()?;
        self.ensure_tracker_bound()?;
        let states = self.states.as_mut().expect("touched-state tracker");
        states.clear();
        if states.try_reserve(state_ptrs.len()).is_err() {
            self.memory_limit_exceeded = true;
            return Err(states
                .allocator()
                .allocation_error("reserve aggregate touched states"));
        }
        states.extend_from_slice(state_ptrs);
        states.sort_unstable();
        states.dedup();
        Ok(states)
    }

    pub(super) fn ensure_available(&self) -> Result<(), String> {
        if self.memory_limit_exceeded {
            Err("aggregate touched-state memory limit was previously exceeded".to_string())
        } else {
            Ok(())
        }
    }

    fn ensure_tracker_bound(&mut self) -> Result<(), String> {
        if self.states.is_none() {
            self.set_tracker(MemTracker::new_child(
                "AggregateTouchedStatesUnbounded",
                &process_mem_tracker(),
            ))?;
        }
        Ok(())
    }
}

pub(super) struct AggregateOperatorVectorsMemory {
    memory_limit_exceeded: bool,
}

impl AggregateOperatorVectorsMemory {
    pub(super) const fn new() -> Self {
        Self {
            memory_limit_exceeded: false,
        }
    }

    pub(super) fn set_tracker(
        &mut self,
        group_states: &mut AggregateStatePointers,
        state_ptrs: &mut AggregateStatePointers,
        tracker: Arc<MemTracker>,
    ) -> Result<(), String> {
        group_states.bind(MemTracker::new_child(
            "AggregateGroupStatePointers",
            &tracker,
        ))?;
        state_ptrs.bind(MemTracker::new_child(
            "AggregateBatchStatePointers",
            &tracker,
        ))?;
        Ok(())
    }

    pub(super) fn ensure_bound(
        &mut self,
        group_states: &mut AggregateStatePointers,
        state_ptrs: &mut AggregateStatePointers,
    ) -> Result<(), String> {
        self.ensure_available()?;
        if !group_states.is_bound() || !state_ptrs.is_bound() {
            let tracker =
                MemTracker::new_child("AggregateOperatorVectorsUnbounded", &process_mem_tracker());
            self.set_tracker(group_states, state_ptrs, tracker)?;
        }
        Ok(())
    }

    pub(super) fn reserve_group_states(
        &mut self,
        group_states: &mut AggregateStatePointers,
        _state_ptrs: &AggregateStatePointers,
        additional: usize,
    ) -> Result<(), String> {
        self.ensure_available()?;
        let values = group_states.values_mut()?;
        if values.try_reserve(additional).is_err() {
            self.memory_limit_exceeded = true;
            return Err(values
                .allocator()
                .allocation_error("reserve aggregate group states"));
        }
        Ok(())
    }

    pub(super) fn reserve_state_ptrs(
        &mut self,
        _group_states: &AggregateStatePointers,
        state_ptrs: &mut AggregateStatePointers,
        additional: usize,
    ) -> Result<(), String> {
        self.ensure_available()?;
        let values = state_ptrs.values_mut()?;
        if values.try_reserve(additional).is_err() {
            self.memory_limit_exceeded = true;
            return Err(values
                .allocator()
                .allocation_error("reserve aggregate batch state pointers"));
        }
        Ok(())
    }

    pub(super) fn ensure_available(&self) -> Result<(), String> {
        if self.memory_limit_exceeded {
            Err("aggregate operator vector memory limit was previously exceeded".to_string())
        } else {
            Ok(())
        }
    }
}

pub(super) struct AggregateStatePointers {
    values: Option<AggregateVec<AggStatePtr>>,
}

impl AggregateStatePointers {
    pub(super) const fn new() -> Self {
        Self { values: None }
    }

    fn bind(&mut self, tracker: Arc<MemTracker>) -> Result<(), String> {
        if self.values.is_some() {
            return Err("aggregate state-pointer vector cannot be rebound".to_string());
        }
        self.values = Some(AggregateVec::new_in(AggregateAllocator::new(tracker)));
        Ok(())
    }

    pub(super) const fn is_bound(&self) -> bool {
        self.values.is_some()
    }

    fn values_mut(&mut self) -> Result<&mut AggregateVec<AggStatePtr>, String> {
        self.values
            .as_mut()
            .ok_or_else(|| "aggregate state-pointer vector tracker is not bound".to_string())
    }
}

impl Deref for AggregateStatePointers {
    type Target = AggregateVec<AggStatePtr>;

    fn deref(&self) -> &Self::Target {
        self.values
            .as_ref()
            .expect("aggregate state-pointer vector tracker")
    }
}

impl DerefMut for AggregateStatePointers {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.values
            .as_mut()
            .expect("aggregate state-pointer vector tracker")
    }
}

fn retained_sum(kernel: &AggKernelEntry, states: &[AggStatePtr]) -> Result<usize, String> {
    states.iter().try_fold(0usize, |total, state| {
        total
            .checked_add(kernel.retained_bytes(*state))
            .ok_or_else(|| "aggregate retained-memory sum overflowed usize".to_string())
    })
}

fn combine_operation_and_accounting<T>(
    operation: Result<T, String>,
    accounting: Result<(), String>,
) -> Result<T, String> {
    match (operation, accounting) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(operation), Err(accounting)) => Err(format!(
            "{operation}; aggregate retained-memory accounting also failed: {accounting}"
        )),
    }
}

fn bytes_to_i64(bytes: usize) -> i64 {
    i64::try_from(bytes).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        AggregateOperatorVectorsMemory, AggregateRetainedMemory, AggregateStatePointers,
        TouchedAggregateStates,
    };
    use crate::runtime::mem_tracker::MemTracker;

    #[test]
    fn touched_state_capacity_is_latched_and_released_after_limit_crossing() {
        let tracker = MemTracker::new_root("query");
        tracker.install_limit_once(1).expect("install limit");
        {
            let mut touched = TouchedAggregateStates::new();
            touched
                .set_tracker(MemTracker::new_child("touched", &tracker))
                .expect("bind tracker");
            let error = touched
                .pointers(&[3, 1, 3, 2])
                .expect_err("touched state allocation must cross the limit");
            assert!(error.contains("ResourceExhausted"), "{error}");
            assert_eq!(tracker.current(), 0);
            let retry = touched
                .pointers(&[1])
                .expect_err("crossing must latch future mutations");
            assert!(retry.contains("previously exceeded"), "{retry}");
            assert_eq!(tracker.current(), 0);
        }
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn operator_vector_capacity_is_latched_and_released_after_limit_crossing() {
        let tracker = MemTracker::new_root("query");
        tracker.install_limit_once(1).expect("install limit");
        {
            let mut memory = AggregateOperatorVectorsMemory::new();
            let mut groups = AggregateStatePointers::new();
            let mut pointers = AggregateStatePointers::new();
            memory
                .set_tracker(
                    &mut groups,
                    &mut pointers,
                    MemTracker::new_child("vectors", &tracker),
                )
                .expect("bind tracker");
            let error = memory
                .reserve_group_states(&mut groups, &pointers, 1)
                .expect_err("group vector allocation must cross the limit");
            assert!(error.contains("ResourceExhausted"), "{error}");
            assert_eq!(tracker.current(), 0);
            let retry = memory
                .reserve_group_states(&mut groups, &pointers, 1)
                .expect_err("crossing must latch future mutations");
            assert!(retry.contains("previously exceeded"), "{retry}");
            assert_eq!(tracker.current(), 0);
        }
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn retained_tracker_requires_binding_before_reservation_and_cannot_rebind() {
        let root = MemTracker::new_root("query");
        let first = MemTracker::new_child("first", &root);
        let second = MemTracker::new_child("second", &root);
        let mut memory = AggregateRetainedMemory::new();
        memory.set_tracker(Arc::clone(&first)).expect("first bind");
        let error = memory
            .set_tracker(second)
            .expect_err("retained tracker rebind must fail");
        assert!(error.contains("cannot be rebound"), "{error}");

        let mut late = AggregateRetainedMemory::new();
        late.reserve_headroom(1).expect("untracked reservation");
        let error = late
            .set_tracker(first)
            .expect_err("late retained tracker bind must fail");
        assert!(error.contains("before allocation"), "{error}");
        late.release_all();
    }
}
