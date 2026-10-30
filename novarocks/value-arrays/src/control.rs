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

//! Synchronous caller-owned control and allocation observations.
//!
//! Callbacks may refuse work and preserve their original error. They do not
//! yield, grant implicit capacity, or retain a continuation in the value layer.

pub type Work<'a, E> = dyn FnMut(usize) -> Result<(), E> + 'a;
pub type Growth<'a, E> = dyn FnMut(std::alloc::Layout) -> Result<(), E> + 'a;

pub fn uncontrolled(_: usize) -> Result<(), std::convert::Infallible> {
    Ok(())
}
pub fn global_growth(_: std::alloc::Layout) -> Result<(), std::convert::Infallible> {
    Ok(())
}

/// New structure at an actual publication site. Buffer payloads and descriptor
/// tables remain Growth observations, separate from these small owners.
#[derive(Clone, Copy, Debug)]
pub enum ValueStructure {
    SharedOwner(std::alloc::Layout),
    ByteArray {
        layout: crate::ByteLayout,
        new_buffer_owners: usize,
    },
}
pub type StructureObserver<'a, E> = dyn FnMut(ValueStructure) -> Result<(), E> + 'a;

pub(crate) fn shared_owner<T, E>(
    observe: &mut StructureObserver<'_, E>,
) -> Result<(), crate::ValueFailure<E>> {
    // Arc's counters precede the aligned public payload. This is authored here
    // for private value-layer types rather than mirrored by a consumer.
    let (layout, _) = std::alloc::Layout::new::<[usize; 2]>()
        .extend(std::alloc::Layout::new::<T>())
        .map_err(|_| crate::ValueError::Invariant(crate::ValueViolation::Bounds))?;
    observe(ValueStructure::SharedOwner(layout.pad_to_align())).map_err(crate::ValueFailure::Caller)
}

// This is the actual Arc slice allocated by publication, independent of the
// temporary Vec table observed through Growth. Empty Arc slices allocate too.
pub(crate) fn shared_slice_owner<T, E>(
    count: usize,
    observe: &mut StructureObserver<'_, E>,
) -> Result<(), crate::ValueFailure<E>> {
    use novarocks_type_contract::owned_resources::layout::{LayoutResourceError, arc_layout};
    let payload = std::alloc::Layout::array::<T>(count)
        .map_err(|_| crate::ValueError::Layout(LayoutResourceError::Arithmetic))?;
    let layout = arc_layout(payload).map_err(crate::ValueError::Layout)?;
    observe(ValueStructure::SharedOwner(layout)).map_err(crate::ValueFailure::Caller)
}
