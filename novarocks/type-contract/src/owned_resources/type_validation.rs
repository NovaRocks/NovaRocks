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

//! Request geometry of the original type-validation pending storage.
//! Neither the fixed scratch nor the dynamic request bound grants memory.
//! Callers admit initialization and the complete original operation separately.

use arrow_schema::DataType;
use std::{alloc::Layout, mem::MaybeUninit};

/// The original borrowed traversal stack, including every slot's occupancy.
pub type TypeValidationScratch<'a> = [Option<(&'a DataType, usize)>; crate::MAX_VALUE_TYPE_NODES];

/// Actual backing layout; this function does not initialize or allocate it.
pub fn scratch_layout() -> Layout {
    Layout::new::<TypeValidationScratch<'_>>()
}

/// Byte work for one opaque initialization of the complete fixed backing.
/// Slot count is not the byte cost, and no internal cooperation is claimed.
pub fn scratch_work_upper_bound() -> usize {
    scratch_layout().size()
}

/// Initialize the exact borrowed backing through completed bounded writes.
/// This is recipe work, not a request for heap memory or a byte-work preload.
pub fn initialize_scratch_observed<'a, 'storage>(
    storage: &'storage mut MaybeUninit<TypeValidationScratch<'a>>,
    work: &mut crate::CompileCheckpoints<'_>,
) -> Result<&'storage mut TypeValidationScratch<'a>, crate::CompileControlError> {
    work.flush()?;
    let slots = storage.as_mut_ptr().cast::<Option<(&'a DataType, usize)>>();
    for index in 0..crate::MAX_VALUE_TYPE_NODES {
        // SAFETY: the exact original scratch backing has this many contiguous
        // slots. Each write initializes one slot without reading old contents.
        unsafe { slots.add(index).write(None) };
        work.step()?;
    }
    work.flush()?;
    // SAFETY: every slot was initialized above. On any refusal this borrow is
    // never published; Option contains only borrowed references and has no Drop.
    Ok(unsafe { storage.assume_init_mut() })
}

/// Cumulative heap-request upper bound for the original dynamic validator.
/// Its initial `vec![(root, 1)]` requests one element. The sole walk checks
/// visited + pending before every child push, so pending length never exceeds
/// MAX_VALUE_TYPE_NODES, including on a type or observer error. Pops cannot
/// increase capacity. The locked growth sequence after the exact one-element
/// backing is a subsequence of the original fresh-push sequence starting at
/// four elements. Include both, without replacing or pre-running validation.
///
/// This deliberately bounds every original shape; it does not inspect a
/// foreign Vec's capacity or claim that the maximum is actually allocated.
/// Element payloads are borrowed. Each emitted Layout is one possible original
/// request contribution, including the initial request; no tail is included.
pub fn original_dynamic_pending_allocation_requests_observed<
    E: From<crate::ControlResourceError>,
>(
    observe: &mut impl FnMut() -> Result<(), E>,
    allocations: &mut Option<super::metadata_materialization::MetadataAllocationLoan<'_, E>>,
) -> Result<usize, E> {
    if !super::profile::LOCKED_TOOLCHAIN {
        return Err(crate::ControlResourceError::SourceModel(
            "Type validation pending source model drift",
        )
        .into());
    }
    let initial = Layout::new::<(&DataType, usize)>();
    if let Some(allocations) = allocations.as_deref_mut() {
        allocations(initial, 1)?;
    }
    observe()?;
    let growth = super::vec::original_fresh_push_allocation_requests_observed::<
        (&DataType, usize),
        E,
    >(crate::MAX_VALUE_TYPE_NODES, observe, allocations)?;
    initial.size().checked_add(growth).ok_or_else(|| {
        E::from(crate::ControlResourceError::from(
            crate::CompileControlError::ResourceExhausted,
        ))
    })
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod scratch_tests {
    use super::*;
    use crate::{CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl};
    use std::sync::Mutex;

    struct Control {
        trace: Mutex<Vec<u32>>,
        refusal: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            let mut trace = self.trace.lock().unwrap();
            trace.push(units);
            if let Some((at, error)) = self.refusal
                && trace.len() == at
            {
                return Err(error);
            }
            Ok(())
        }
    }
    #[test]
    fn project_metadata_scratch_initializes_exact_original_backing_with_bounded_write_observation()
    {
        let control = Control {
            trace: Mutex::default(),
            refusal: None,
        };
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
        let mut storage = MaybeUninit::uninit();
        let scratch = initialize_scratch_observed(&mut storage, &mut work).unwrap();
        assert!(scratch.iter().all(Option::is_none));
        assert_eq!(std::mem::size_of_val(scratch), scratch_work_upper_bound());
        let trace = control.trace.lock().unwrap();
        assert_eq!(
            trace.iter().sum::<u32>() as usize,
            crate::MAX_VALUE_TYPE_NODES
        );
        assert!(
            trace
                .iter()
                .all(|units| *units <= crate::MAX_UNOBSERVED_COMPILE_WORK)
        );
    }
    #[test]
    fn project_metadata_scratch_refuses_before_initialization_and_keeps_each_actual_written_prefix()
    {
        let ty = DataType::Int64;
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            // Entry, first interior quantum, and final post-initialization tail.
            for (at, completed) in [(2, 0), (3, 256), (19, crate::MAX_VALUE_TYPE_NODES)] {
                let control = Control {
                    trace: Mutex::default(),
                    refusal: Some((at, error)),
                };
                let mut work =
                    CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
                // Sentinels let this test inspect the actual untouched suffix.
                let mut storage = MaybeUninit::new([Some((&ty, 7)); crate::MAX_VALUE_TYPE_NODES]);
                assert!(
                    matches!(initialize_scratch_observed(&mut storage, &mut work), Err(actual) if actual == error)
                );
                // SAFETY: test storage started fully initialized; the helper
                // only replaces initialized slots with initialized None values.
                let slots = unsafe { storage.assume_init_ref() };
                assert!(slots[..completed].iter().all(Option::is_none));
                assert!(
                    slots[completed..]
                        .iter()
                        .all(|slot| *slot == Some((&ty, 7)))
                );
                assert_eq!(control.trace.lock().unwrap().len(), at);
                assert_eq!(work.flush(), Err(error));
                assert_eq!(control.trace.lock().unwrap().len(), at);
            }
        }
    }
}
