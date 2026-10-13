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

//! Private scalar whole-invocation transport. No scalar ABI is installed here.
//! A caller must supply the actual invocation and its already-admitted original
//! diagnostic scope. Selection alone never decides whether a call is activated.
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::aggregate_invocation_backing::HostShared;
use crate::kernel_input::EvaluationCheckpoints;
use crate::opaque_memory::OpaqueReservation;
use crate::{KernelFailure, ScalarCallContract, Selection};
use allocator_api2::vec::Vec as HostVec;
use std::{
    fmt,
    sync::{Arc, OnceLock},
};

enum SourceDomain {
    All {
        batch_rows: usize,
    },
    Sparse {
        batch_rows: usize,
        rows: HostVec<usize, HostAggregateAllocator>,
    },
    DirectSparse {
        batch_rows: usize,
        // Field destruction order keeps the actual grant live through Vec Drop.
        rows: Vec<usize>,
        scope: Option<OpaqueReservation>,
    },
}
impl SourceDomain {
    fn batch_rows(&self) -> usize {
        match self {
            Self::All { batch_rows }
            | Self::Sparse { batch_rows, .. }
            | Self::DirectSparse { batch_rows, .. } => *batch_rows,
        }
    }
    fn len(&self) -> usize {
        match self {
            Self::All { batch_rows } => *batch_rows,
            Self::Sparse { rows, .. } => rows.len(),
            Self::DirectSparse { rows, .. } => rows.len(),
        }
    }
    fn row(&self, ordinal: usize) -> Option<usize> {
        match self {
            Self::All { batch_rows } => (ordinal < *batch_rows).then_some(ordinal),
            Self::Sparse { rows, .. } => rows.get(ordinal).copied(),
            Self::DirectSparse { rows, .. } => rows.get(ordinal).copied(),
        }
    }
    fn bytes(&self) -> usize {
        match self {
            Self::All { .. } => 0,
            Self::Sparse { rows, .. } => rows.capacity() * std::mem::size_of::<usize>(),
            Self::DirectSparse { rows, .. } => rows.capacity() * std::mem::size_of::<usize>(),
        }
    }
    fn opaque_admission_bytes(&self) -> usize {
        match self {
            Self::DirectSparse {
                scope: Some(scope), ..
            } => scope.remaining_bytes(),
            _ => 0,
        }
    }
}
struct ScalarDataInner {
    contract: Arc<ScalarCallContract>,
    domain: SourceDomain,
    allocator_metadata_bytes: usize,
    original: OnceLock<String>,
    // Destroy the moved String before releasing its actual host reservation.
    diagnostic_scope: OpaqueReservation,
}
#[derive(Clone)]
pub struct ScalarInvocationData(HostShared<ScalarDataInner>);
impl ScalarInvocationData {
    pub fn contract(&self) -> &Arc<ScalarCallContract> {
        &self.0.contract
    }
    pub fn message(&self) -> &str {
        self.0
            .original
            .get()
            .expect("published original scalar Data")
    }
    pub fn batch_rows(&self) -> usize {
        self.0.domain.batch_rows()
    }
    pub fn source_len(&self) -> usize {
        self.0.domain.len()
    }
    /// Source-domain evidence, never the ordinal of a maskable row error.
    pub fn source_row(&self, ordinal: usize) -> Option<usize> {
        self.0.domain.row(ordinal)
    }
    pub fn same_backing(&self, other: &Self) -> bool {
        self.0.ptr_eq(&other.0)
    }
    pub fn retained_backing_bytes(&self) -> usize {
        self.0.allocator_metadata_bytes
            + self.0.block_bytes()
            + self.0.domain.bytes()
            + self.0.original.get().expect("published Data").capacity()
    }
    pub fn retained_admission_bytes(&self) -> usize {
        self.0.diagnostic_scope.remaining_bytes() + self.0.domain.opaque_admission_bytes()
    }
}
impl PartialEq for ScalarInvocationData {
    fn eq(&self, other: &Self) -> bool {
        self.same_backing(other)
    }
}
impl Eq for ScalarInvocationData {}
impl fmt::Debug for ScalarInvocationData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScalarInvocationData")
            .field("context", &self.0.contract.context())
            .field("batch_rows", &self.batch_rows())
            .field("source_len", &self.source_len())
            .field("message", &self.message())
            .finish()
    }
}
impl fmt::Display for ScalarInvocationData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message())
    }
}
impl std::error::Error for ScalarInvocationData {}

/// A private slot for one actual invoking call. It is not an activation receipt,
/// a prepared diagnostic, a scalar instance, or an implicit empty-domain call.
pub(super) struct ScalarDataSlot(HostShared<ScalarDataInner>);

/// A virgin direct slot cannot be cloned or published through the old borrowed
/// slot interface. Its sole writer consumes it before exposing any Data alias.
pub(super) struct DirectScalarDataSlot(HostShared<ScalarDataInner>);

/// The exact requests of the original direct holder, before constructing it.
/// The complete String is moved into this holder and has a separate author.
pub(crate) fn direct_slot_request_layouts(
    selection: Selection<'_>,
) -> Result<(std::alloc::Layout, Option<std::alloc::Layout>), KernelFailure> {
    let rows = if selection.is_all() || selection.is_empty() {
        None
    } else {
        Some(
            std::alloc::Layout::array::<usize>(selection.len())
                .map_err(|_| KernelFailure::ResourceExhausted)?,
        )
    };
    Ok((HostShared::<ScalarDataInner>::allocation_layout(), rows))
}
impl DirectScalarDataSlot {
    pub(super) fn publish_original(self, message: String) -> ScalarInvocationData {
        // No handle or reference to the cell escaped construction. This is the
        // only set on this cell, so the pinned OnceLock author cannot wait.
        let _ = self.0.original.set(message);
        ScalarInvocationData(self.0)
    }
}
impl ScalarDataSlot {
    pub(super) fn prepare_direct_for_actual_invocation(
        contract: Arc<ScalarCallContract>,
        selection: Selection<'_>,
        allocator: &HostAggregateAllocator,
        diagnostic_scope: OpaqueReservation,
        source_scope: Option<OpaqueReservation>,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<DirectScalarDataSlot, KernelFailure> {
        if !diagnostic_scope.belongs_to_allocator(allocator) {
            return Err(crate::kernel_control::invalid(
                "scalar diagnostic scope uses another host authority",
            ));
        }
        let (_, source_layout) = direct_slot_request_layouts(selection)?;
        let source_bytes = source_layout.map_or(0, |layout| layout.size());
        match &source_scope {
            Some(scope)
                if source_bytes != 0
                    && scope.belongs_to_allocator(allocator)
                    && scope.remaining_bytes() == source_bytes => {}
            None if source_bytes == 0 => {}
            _ => {
                return Err(crate::kernel_control::invalid(
                    "scalar source scope differs from its exact host layout",
                ));
            }
        }
        let domain = if selection.is_all() {
            SourceDomain::All {
                batch_rows: selection.batch_rows(),
            }
        } else {
            // Move the scope into its holder before allocating. Every early
            // return now drops the backing before relinquishing its grant.
            let mut domain = SourceDomain::DirectSparse {
                batch_rows: selection.batch_rows(),
                rows: Vec::new(),
                scope: source_scope,
            };
            let SourceDomain::DirectSparse { rows, .. } = &mut domain else {
                unreachable!("direct sparse source holder");
            };
            if !selection.is_empty() {
                work.flush()?;
                rows.try_reserve_exact(selection.len())
                    .map_err(|_| KernelFailure::ResourceExhausted)?;
                work.flush()?;
            }
            for row in selection.iter() {
                rows.push(row);
                work.step()?;
            }
            domain
        };
        Ok(DirectScalarDataSlot(HostShared::try_new_direct(
            ScalarDataInner {
                contract,
                domain,
                allocator_metadata_bytes: allocator.metadata_bytes(),
                original: OnceLock::new(),
                diagnostic_scope,
            },
            allocator.clone(),
            work,
        )?))
    }
    pub(super) fn prepare_for_actual_invocation(
        contract: Arc<ScalarCallContract>,
        selection: Selection<'_>,
        allocator: &HostAggregateAllocator,
        diagnostic_scope: OpaqueReservation,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Self, KernelFailure> {
        if !diagnostic_scope.belongs_to_allocator(allocator) {
            return Err(crate::kernel_control::invalid(
                "scalar diagnostic scope uses another host authority",
            ));
        }
        let domain = if selection.is_all() {
            SourceDomain::All {
                batch_rows: selection.batch_rows(),
            }
        } else {
            let mut rows = HostVec::new_in(allocator.clone());
            if !selection.is_empty() {
                work.flush()?;
                rows.try_reserve_exact(selection.len())
                    .map_err(|_| allocator.take_failure())?;
                work.flush()?;
            }
            for row in selection.iter() {
                rows.push(row);
                work.step()?;
            }
            SourceDomain::Sparse {
                batch_rows: selection.batch_rows(),
                rows,
            }
        };
        Ok(Self(HostShared::try_new(
            ScalarDataInner {
                contract,
                domain,
                allocator_metadata_bytes: allocator.metadata_bytes(),
                original: OnceLock::new(),
                diagnostic_scope,
            },
            allocator.clone(),
            work,
        )?))
    }
    /// The original String must have been constructed under diagnostic_scope's
    /// proven source bound. This transport neither grants nor checks that bound
    /// after the original Data error. It performs no copy/formatter/allocation,
    /// callback or fallible footer. A producer with no such proof cannot use it.
    pub(super) fn publish_original(&self, message: String) -> ScalarInvocationData {
        let _ = self.0.original.set(message);
        ScalarInvocationData(self.0.clone())
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScalarInvocationFailure {
    Kernel(KernelFailure),
    Data(ScalarInvocationData),
}

impl From<KernelFailure> for ScalarInvocationFailure {
    fn from(cause: KernelFailure) -> Self {
        Self::Kernel(cause)
    }
}
impl fmt::Display for ScalarInvocationFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Kernel(cause) => cause.fmt(f),
            Self::Data(data) => data.fmt(f),
        }
    }
}
impl std::error::Error for ScalarInvocationFailure {}

/// Private lifecycle probe, not a replacement for the shared scalar wrapper.
/// The public ABI/instance author will consume this nominal result separately.
pub(super) fn finish_invocation<T>(
    result: Result<T, ScalarInvocationFailure>,
    post: impl FnOnce() -> Result<(), KernelFailure>,
) -> Result<T, ScalarInvocationFailure> {
    match result {
        Err(cause) => Err(cause),
        Ok(value) => post()
            .map(|()| value)
            .map_err(ScalarInvocationFailure::Kernel),
    }
}
/// One mutable instance's failure bit. The caller must pass deferred operations;
/// already evaluating them before this author would replay a failed instance.
#[derive(Default)]
pub(super) struct ScalarInvocationLatch {
    failed: bool,
}
impl ScalarInvocationLatch {
    pub(super) fn execute<T>(
        &mut self,
        operation: impl FnOnce() -> Result<T, ScalarInvocationFailure>,
        post: impl FnOnce() -> Result<(), KernelFailure>,
    ) -> Result<T, ScalarInvocationFailure> {
        if self.failed {
            return Err(ScalarInvocationFailure::Kernel(
                KernelFailure::InstanceFailed,
            ));
        }
        let result = finish_invocation(operation(), post);
        if result.is_err() {
            self.failed = true;
        }
        result
    }
}
#[cfg(test)]
#[path = "scalar_invocation_data_tests.rs"]
mod tests;

#[cfg(test)]
mod request_layout_tests {
    use super::*;
    use crate::opaque_memory::{OpaqueAllocationHost, OpaqueRetainedCharge};
    use crate::source_custody_resources_tests::measured;
    use crate::{AggregateStateAllocator, FunctionValueType, KernelEvaluationControl};
    use arrow_schema::DataType;
    use std::{
        alloc::Layout,
        ptr::NonNull,
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    #[derive(Default)]
    struct Host {
        live: AtomicUsize,
        opaque: AtomicUsize,
    }
    impl AggregateStateAllocator for Host {
        fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
            let pointer = NonNull::new(unsafe { std::alloc::alloc(layout) })
                .ok_or(KernelFailure::ResourceExhausted)?;
            self.live.fetch_add(layout.size(), Ordering::Relaxed);
            Ok(pointer)
        }
        unsafe fn release(&self, pointer: NonNull<u8>, layout: Layout) {
            unsafe { std::alloc::dealloc(pointer.as_ptr(), layout) };
            self.live.fetch_sub(layout.size(), Ordering::Relaxed);
        }
        fn opaque_allocation_host(&self) -> Option<&dyn OpaqueAllocationHost> {
            Some(self)
        }
    }
    impl OpaqueAllocationHost for Host {
        fn reserve_opaque(&self, bytes: usize) -> Result<(), KernelFailure> {
            self.opaque.fetch_add(bytes, Ordering::Relaxed);
            Ok(())
        }
        fn release_opaque(&self, bytes: usize) {
            self.opaque.fetch_sub(bytes, Ordering::Relaxed);
        }
    }
    struct Control;
    impl KernelEvaluationControl for Control {
        fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
            Ok(())
        }
        fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
            panic!("direct slot construction never waits")
        }
    }

    #[test]
    fn invocation_scalar_resources_direct_slot_matches_independent_original_requests() {
        // This original prepared author supplies only the immutable carrier
        // contract. This is a slot request probe, not a JSON funding receipt.
        let prepared = super::super::string_md5_owner::prepared_for_test_with_policy(
            "md5",
            &[FunctionValueType::new(DataType::Utf8, true)],
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        )
        .unwrap();
        let rows: Vec<_> = (0..321).map(|i| 2 * i).collect();
        for selection in [
            Selection::all(9),
            Selection::all(0),
            Selection::try_sparse(1000, &rows).unwrap(),
            Selection::try_sparse(9, &[]).unwrap(),
        ] {
            let host = Arc::new(Host::default());
            let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
            let before = host.live.load(Ordering::Relaxed);
            let charge = OpaqueRetainedCharge::try_new(host.clone()).unwrap();
            let diagnostic = charge.reserve_operation(4096).unwrap();
            let source_bytes = if selection.is_all() {
                0
            } else {
                selection.len() * size_of::<usize>()
            };
            let source = if source_bytes == 0 {
                None
            } else {
                Some(charge.reserve_operation(source_bytes).unwrap())
            };
            let (bound, preparation) = measured(|| {
                crate::invocation_scalar_resources::direct_data_slot_geometry(selection)
            });
            assert_eq!(preparation, (0, 0, 0, 0));
            let bound = bound.unwrap();
            let mut work = EvaluationCheckpoints::new(&Control);
            let (slot, actual) = measured(|| {
                ScalarDataSlot::prepare_direct_for_actual_invocation(
                    prepared.contract().clone(),
                    selection,
                    &allocator,
                    diagnostic,
                    source,
                    &mut work,
                )
            });
            let slot = slot.unwrap();
            assert_eq!(actual.0, bound.requested_bytes);
            assert_eq!(actual.1, bound.requests);
            assert_eq!(actual.2, bound.peak_bytes);
            assert_eq!(actual.3, bound.requested_bytes);
            drop(slot);
            assert_eq!(host.live.load(Ordering::Relaxed), before);
            assert_eq!(host.opaque.load(Ordering::Relaxed), 0);
            drop(allocator);
            assert_eq!(host.live.load(Ordering::Relaxed), 0);
        }
    }
}
